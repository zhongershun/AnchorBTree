#include <filesystem>
#include <fstream>
#include <iostream>

{
    if(ctx.IsRootPage(target_pid)){ // 当前的节点即是叶子节点有是根节点
      if(target_page->GetSize()!=0){
        return true;
      }else{
        auto header_page = ctx.write_header_page_.value().AsMut<BPlusTreeHeaderPage>();
        header_page->root_page_id_=INVALID_PAGE_ID;
        return true;
      }
    }
    // 当前节点不是根节点
    // write_set_ [(...)parent, target]
    // auto leaf_page = reinterpret_cast<LeafPage*>(target_page);
    auto parent_page = reinterpret_cast<InternalPage*>(ctx.ParentPage());
    int target_idx = parent_page->ValueIndex(target_pid);
    if(target_page->GetSize()==0){
      if(parent_page->CanDelete()){
        parent_page->RemoveInner(target_idx);
        ctx.CtxPopBackWritePage();
        return true;
      }else{
        parent_page->RemoveInner(target_idx);
        ctx.CtxPopBackWritePage();
        return CoalesceOrRedistribute(ctx);
      }
    }
    int neighbor_idx;
    bool from_prev = true;
    if(target_idx==0){
      neighbor_idx = target_idx+1;
      from_prev = false;
    }else{
      neighbor_idx = target_idx-1;
      from_prev = true;
    }
    if(neighbor_idx>=parent_page->GetSize()){
      // only when parent->GetSize()==1
      ctx.CtxPopBackWritePage();
      return CoalesceOrRedistribute(ctx);
    }
    page_id_t neighbor_pid = parent_page->ValueAt(neighbor_idx);
    auto neighbor_guard = bpm_->WritePage(neighbor_pid);
    auto neighbor_page = neighbor_guard.AsMut<LeafPage>();
    ctx.CtxWritePage(std::move(neighbor_guard));
    if(neighbor_page->CanDelete()){
      // write_set_ [(...)parent, target, neighbor]
      if(from_prev){
        parent_page->SetKeyAt(target_idx,neighbor_page->KeyAt(neighbor_page->GetSize()-1));
      }else{
        parent_page->SetKeyAt(neighbor_idx,neighbor_page->KeyAt(1));
      }
      return Redistribute(ctx, from_prev);
    }else{
      // write_set_ [(...)parent, target, neighbor]
      Coalesce(ctx, from_prev);
      if(from_prev){ // remove target
        parent_page->RemoveInner(target_idx);
      }else{ // remove neighbor
        parent_page->RemoveInner(neighbor_idx);
      }
      ctx.CtxPopBackWritePage();
      ctx.CtxPopBackWritePage();
      // write_set_ [(...)parent]
      if(parent_page->GetSize()>=parent_page->GetMinSize()){
        return true;
      }else{
        return CoalesceOrRedistribute(ctx);
      }
    }
}

{
    if(ctx.IsRootPage(target_pid)){ // 当前的节点是根节点
      if(target_page->GetSize()!=0){
        return true;
      }else{
        auto header_page = ctx.write_header_page_.value().AsMut<BPlusTreeHeaderPage>();
        header_page->root_page_id_=INVALID_PAGE_ID;
        return true;
      }
    }
    // 当前节点不是根节点
    // write_set_ [(...)parent, target]
    // auto leaf_page = reinterpret_cast<InternalPage*>(target_page);
    auto parent_page = reinterpret_cast<InternalPage*>(ctx.ParentPage());
    int target_idx = parent_page->ValueIndex(target_pid);
    if(target_page->GetSize()==0){
      if(parent_page->CanDelete()){
        parent_page->RemoveInner(target_idx);
        ctx.CtxPopBackWritePage();
        return true;
      }else{
        parent_page->RemoveInner(target_idx);
        ctx.CtxPopBackWritePage();
        return CoalesceOrRedistribute(ctx);
      }
    }
    int neighbor_idx;
    bool from_prev = true;
    if(target_idx==0){
      neighbor_idx = target_idx+1;
      from_prev = false;
    }else{
      neighbor_idx = target_idx-1;
      from_prev = true;
    }
    if(neighbor_idx>=parent_page->GetSize()){
      // only when parent->GetSize()==1
      ctx.CtxPopBackWritePage();
      return CoalesceOrRedistribute(ctx);
    }
    page_id_t neighbor_pid = parent_page->ValueAt(neighbor_idx);
    auto neighbor_guard = bpm_->WritePage(neighbor_pid);
    auto neighbor_page = neighbor_guard.AsMut<InternalPage>();
    ctx.CtxWritePage(std::move(neighbor_guard));
    if(neighbor_page->CanDelete()){
      // write_set_ [(...)parent, target, neighbor]
      if(from_prev){
        parent_page->SetKeyAt(target_idx,neighbor_page->KeyAt(neighbor_page->GetSize()-1));
      }else{
        parent_page->SetKeyAt(neighbor_idx,neighbor_page->KeyAt(1));
      }
      return Redistribute(ctx, from_prev);
    }else{
      // write_set_ [(...)parent, target, neighbor]
      Coalesce(ctx, from_prev);
      if(from_prev){ // remove target
        parent_page->RemoveInner(target_idx);
      }else{ // remove neighbor
        parent_page->RemoveInner(neighbor_idx);
      }
      ctx.CtxPopBackWritePage();
      ctx.CtxPopBackWritePage();
      // write_set_ [(...)parent]
      if(parent_page->GetSize()>=parent_page->GetMinSize()){
        return true;
      }else{
        return CoalesceOrRedistribute(ctx);
      }
    }
}